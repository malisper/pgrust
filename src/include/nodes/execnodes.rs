use crate::backend::access::heap::heapam::VisibleHeapScan;
use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_end, heap_scan_page_next_tuple, heap_scan_prepare_next_page};
use crate::include::access::htup::{AttributeDesc, HeapTuple, ItemPointerData};
use crate::pgrust::compact_string::CompactString;
use crate::backend::parser::{SqlType, SqlTypeKind, SubqueryComparisonOp};
use crate::{OwnedBufferPin, RelFileLocator, SmgrStorageBackend};
use num_bigint::BigInt;
use num_integer::Integer;
use num_traits::{Signed, Zero};
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::backend::executor::exec_expr::{compare_order_by_keys, decode_value, eval_expr};
use crate::backend::executor::{AccumState, AggGroup, ExecError, ExecutorContext};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarType {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Numeric,
    Json,
    Text,
    Bool,
    Array(Box<ScalarType>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub storage: AttributeDesc,
    pub ty: ScalarType,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDesc {
    pub columns: Vec<ColumnDesc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryColumn {
    pub name: String,
    pub sql_type: SqlType,
}

impl QueryColumn {
    pub fn text(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql_type: SqlType::new(SqlTypeKind::Text),
        }
    }
}

impl RelationDesc {
    pub fn attribute_descs(&self) -> Vec<AttributeDesc> {
        self.columns.iter().map(|c| c.storage.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Numeric(NumericValue),
    Json(CompactString),
    Text(CompactString),
    /// Raw pointer to on-page text bytes. Valid while the buffer page is pinned
    /// (the slot's `Rc<OwnedBufferPin>` keeps the pin alive). User data on the
    /// page is immutable after insertion.
    TextRef(*const u8, u32),
    Bool(bool),
    Array(Vec<Value>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NumericValue {
    Finite { coeff: BigInt, scale: u32 },
    NaN,
}

impl NumericValue {
    pub fn zero() -> Self {
        Self::Finite {
            coeff: BigInt::zero(),
            scale: 0,
        }
    }

    pub fn from_i64(value: i64) -> Self {
        Self::Finite {
            coeff: BigInt::from(value),
            scale: 0,
        }
    }

    pub fn normalize(self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            Self::Finite { mut coeff, mut scale } => {
                if coeff.is_zero() {
                    return Self::zero();
                }
                let ten = BigInt::from(10u8);
                while scale > 0 {
                    let (q, r) = coeff.div_rem(&ten);
                    if !r.is_zero() {
                        break;
                    }
                    coeff = q;
                    scale -= 1;
                }
                Self::Finite { coeff, scale }
            }
        }
    }

    pub fn digit_count(&self) -> i32 {
        match self {
            Self::NaN => 0,
            Self::Finite { coeff, .. } => coeff.to_str_radix(10).trim_start_matches('-').trim_start_matches('0').len().max(1) as i32,
        }
    }

    pub fn negate(&self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            Self::Finite { coeff, scale } => Self::Finite {
                coeff: -coeff.clone(),
                scale: *scale,
            },
        }
    }

    pub fn render(&self) -> String {
        match self {
            Self::NaN => "NaN".to_string(),
            Self::Finite { coeff, scale } => {
                let negative = coeff.is_negative();
                let digits = coeff.abs().to_str_radix(10);
                if *scale == 0 {
                    if negative { format!("-{digits}") } else { digits }
                } else {
                    let scale = *scale as usize;
                    let mut out = String::new();
                    if negative {
                        out.push('-');
                    }
                    if digits.len() <= scale {
                        out.push('0');
                        out.push('.');
                        for _ in 0..(scale - digits.len()) {
                            out.push('0');
                        }
                        out.push_str(&digits);
                    } else {
                        let split = digits.len() - scale;
                        out.push_str(&digits[..split]);
                        out.push('.');
                        out.push_str(&digits[split..]);
                    }
                    out
                }
            }
        }
    }
}

impl From<&str> for NumericValue {
    fn from(value: &str) -> Self {
        parse_numeric_literal(value).unwrap_or_else(NumericValue::zero)
    }
}

impl From<String> for NumericValue {
    fn from(value: String) -> Self {
        NumericValue::from(value.as_str())
    }
}

fn parse_numeric_literal(text: &str) -> Option<NumericValue> {
    if text.eq_ignore_ascii_case("nan") {
        return Some(NumericValue::NaN);
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (mantissa, exponent) = match trimmed.find(['e', 'E']) {
        Some(index) => (&trimmed[..index], trimmed[index + 1..].parse::<i32>().ok()?),
        None => (trimmed, 0),
    };
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
    let parts: Vec<&str> = unsigned.split('.').collect();
    if parts.len() > 2 {
        return None;
    }
    let whole = parts[0];
    let frac = parts.get(1).copied().unwrap_or("");
    if (!whole.is_empty() && !whole.chars().all(|ch| ch.is_ascii_digit()))
        || !frac.chars().all(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{whole}{frac}");
    if digits.is_empty() {
        digits.push('0');
    }
    let mut scale = frac.len() as i32 - exponent;
    if scale < 0 {
        digits.extend(std::iter::repeat_n('0', (-scale) as usize));
        scale = 0;
    }
    let mut coeff = digits.parse::<BigInt>().ok()?;
    if negative {
        coeff = -coeff;
    }
    Some(NumericValue::Finite {
        coeff,
        scale: scale as u32,
    }
    .normalize())
}

impl Value {
    /// Get text content as `&str`, works for both `Text` and `TextRef`.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s.as_str()),
            Value::TextRef(ptr, len) => Some(unsafe {
                std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
            }),
            _ => None,
        }
    }

    /// Convert to an owned `Value`. `TextRef` becomes `Text(CompactString)`;
    /// other variants are cloned cheaply.
    pub fn to_owned_value(&self) -> Value {
        match self {
            Value::Int16(v) => Value::Int16(*v),
            Value::Int32(v) => Value::Int32(*v),
            Value::Int64(v) => Value::Int64(*v),
            Value::Float64(v) => Value::Float64(*v),
            Value::Numeric(v) => Value::Numeric(v.clone()),
            Value::Json(s) => Value::Json(s.clone()),
            Value::TextRef(ptr, len) => {
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
                };
                Value::Text(CompactString::new(s))
            }
            Value::Text(s) => Value::Text(s.clone()),
            Value::Bool(v) => Value::Bool(*v),
            Value::Array(values) => Value::Array(values.iter().map(Value::to_owned_value).collect()),
            Value::Null => Value::Null,
        }
    }

    /// Convert all `TextRef` values in a slice to owned `Text` in place.
    pub fn materialize_all(values: &mut Vec<Value>) {
        for v in values.iter_mut() {
            if let Value::TextRef(ptr, len) = *v {
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len as usize))
                };
                *v = Value::Text(CompactString::new(s));
            } else if let Value::Array(items) = v {
                for item in items.iter_mut() {
                    *item = item.to_owned_value();
                }
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int16(a), Value::Int16(b)) => a == b,
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Int64(a), Value::Int64(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Numeric(a), Value::Numeric(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
                a.as_text().unwrap() == b.as_text().unwrap()
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Value::Int16(v) => { 0u8.hash(state); v.hash(state); }
            Value::Int32(v) => { 1u8.hash(state); v.hash(state); }
            Value::Int64(v) => { 2u8.hash(state); v.hash(state); }
            Value::Float64(v) => { 3u8.hash(state); v.to_bits().hash(state); }
            Value::Numeric(v) => { 4u8.hash(state); v.hash(state); }
            Value::Json(s) => { 9u8.hash(state); s.as_str().hash(state); }
            // Text and TextRef hash the same way so equal values get the same hash
            Value::Text(s) => { 5u8.hash(state); s.as_str().hash(state); }
            Value::TextRef(ptr, len) => {
                5u8.hash(state);
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
                };
                s.hash(state);
            }
            Value::Bool(v) => { 6u8.hash(state); v.hash(state); }
            Value::Array(values) => {
                7u8.hash(state);
                values.hash(state);
            }
            Value::Null => { 8u8.hash(state); }
        }
    }
}

// SAFETY: TextRef points to immutable user data on a pinned buffer page.
// The pin (via Rc<OwnedBufferPin>) ensures the page stays alive. The data
// is never written after insertion (heap_page_replace_tuple only writes headers).
unsafe impl Send for Value {}
unsafe impl Sync for Value {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEntry {
    pub name: String,
    pub expr: Expr,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByEntry {
    pub expr: Expr,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    JsonAgg,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::JsonAgg => "json_agg",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuiltinScalarFunction {
    Random,
    ToJson,
    ArrayToJson,
    JsonTypeof,
    JsonArrayLength,
    JsonExtractPath,
    JsonExtractPathText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonTableFunction {
    ObjectKeys,
    Each,
    EachText,
    ArrayElements,
    ArrayElementsText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggAccum {
    pub func: AggFunc,
    pub arg: Option<Expr>,
    pub distinct: bool,
    pub sql_type: SqlType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(usize),
    OuterColumn { depth: usize, index: usize },
    Const(Value),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),
    UnaryPlus(Box<Expr>),
    Negate(Box<Expr>),
    Cast(Box<Expr>, SqlType),
    Eq(Box<Expr>, Box<Expr>),
    NotEq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    LtEq(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    GtEq(Box<Expr>, Box<Expr>),
    RegexMatch(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    ArrayLiteral {
        elements: Vec<Expr>,
        array_type: SqlType,
    },
    ArrayOverlap(Box<Expr>, Box<Expr>),
    ScalarSubquery(Box<Plan>),
    ExistsSubquery(Box<Plan>),
    AnySubquery {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        subquery: Box<Plan>,
    },
    AllSubquery {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        subquery: Box<Plan>,
    },
    AnyArray {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        right: Box<Expr>,
    },
    AllArray {
        left: Box<Expr>,
        op: SubqueryComparisonOp,
        right: Box<Expr>,
    },
    Random,
    JsonGet(Box<Expr>, Box<Expr>),
    JsonGetText(Box<Expr>, Box<Expr>),
    JsonPath(Box<Expr>, Box<Expr>),
    JsonPathText(Box<Expr>, Box<Expr>),
    FuncCall {
        func: BuiltinScalarFunction,
        args: Vec<Expr>,
    },
    CurrentTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Plan {
    Result,
    SeqScan {
        rel: RelFileLocator,
        desc: RelationDesc,
    },
    NestedLoopJoin {
        left: Box<Plan>,
        right: Box<Plan>,
        on: Expr,
    },
    Filter {
        input: Box<Plan>,
        predicate: Expr,
    },
    OrderBy {
        input: Box<Plan>,
        items: Vec<OrderByEntry>,
    },
    Limit {
        input: Box<Plan>,
        limit: Option<usize>,
        offset: usize,
    },
    Projection {
        input: Box<Plan>,
        targets: Vec<TargetEntry>,
    },
    Aggregate {
        input: Box<Plan>,
        group_by: Vec<Expr>,
        accumulators: Vec<AggAccum>,
        having: Option<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    GenerateSeries {
        start: Expr,
        stop: Expr,
        step: Expr,
        output: QueryColumn,
    },
    Unnest {
        args: Vec<Expr>,
        output_columns: Vec<QueryColumn>,
    },
    JsonTableFunction {
        kind: JsonTableFunction,
        arg: Expr,
        output_columns: Vec<QueryColumn>,
    },
}

impl Plan {
    pub fn columns(&self) -> Vec<QueryColumn> {
        match self {
            Plan::Result => vec![],
            Plan::SeqScan { desc, .. } => desc
                .columns
                .iter()
                .map(|c| QueryColumn {
                    name: c.name.clone(),
                    sql_type: c.sql_type,
                })
                .collect(),
            Plan::Filter { input, .. } | Plan::OrderBy { input, .. } | Plan::Limit { input, .. } => {
                input.columns()
            }
            Plan::Projection { targets, .. } => targets
                .iter()
                .map(|t| QueryColumn {
                    name: t.name.clone(),
                    sql_type: t.sql_type,
                })
                .collect(),
            Plan::Aggregate { output_columns, .. } => output_columns.clone(),
            Plan::NestedLoopJoin { left, right, .. } => {
                let mut cols = left.columns();
                cols.extend(right.columns());
                cols
            }
            Plan::GenerateSeries { output, .. } => vec![output.clone()],
            Plan::Unnest { output_columns, .. } => output_columns.clone(),
            Plan::JsonTableFunction { output_columns, .. } => output_columns.clone(),
        }
    }

    /// Extract output column names from the plan tree.
    pub fn column_names(&self) -> Vec<String> {
        self.columns().into_iter().map(|c| c.name).collect()
    }
}

pub struct TupleSlot {
    pub(crate) kind: SlotKind,
    /// Decoded column values, like PG's tts_values[]. Reused across tuples
    /// to avoid per-tuple allocation.
    pub(crate) tts_values: Vec<Value>,
    /// Number of columns decoded so far (0..ncols). Like PG's tts_nvalid.
    pub(crate) tts_nvalid: usize,
    /// Byte offset in the tuple data area after the last decoded column,
    /// used to resume incremental decode for variable-width columns.
    pub(crate) decode_offset: usize,
    /// Compiled tuple decoder, like PG's tts_tupleDescriptor. Set once when
    /// the slot is created; shared via Rc so future scan types can share it.
    pub(crate) decoder: Option<Rc<super::tuple_decoder::CompiledTupleDecoder>>,
}

/// Describes how the slot's underlying tuple data is stored.
/// Like PG's TTS_FLAG_* / BufferHeapTupleTableSlot vs VirtualTupleTableSlot.
pub(crate) enum SlotKind {
    /// No tuple stored. Initial state before first scan tuple.
    Empty,
    /// tts_values is authoritative (no backing tuple to decode from).
    Virtual,
    /// Owned heap tuple from a heap fetch (used by UPDATE/DELETE path).
    HeapTuple {
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
    },
    /// Zero-copy reference to tuple bytes on a pinned buffer page.
    /// Decoded lazily into tts_values via the slot's `decoder` field.
    BufferHeapTuple {
        tuple_ptr: *const u8,
        tuple_len: usize,
        pin: Rc<OwnedBufferPin<SmgrStorageBackend>>,
    },
}

impl std::fmt::Debug for SlotKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotKind::Empty => write!(f, "Empty"),
            SlotKind::Virtual => write!(f, "Virtual"),
            SlotKind::HeapTuple { tid, .. } => f.debug_struct("HeapTuple").field("tid", tid).finish(),
            SlotKind::BufferHeapTuple { tuple_len, pin, .. } => f
                .debug_struct("BufferHeapTuple")
                .field("tuple_len", tuple_len)
                .field("buffer_id", &pin.buffer_id())
                .finish(),
        }
    }
}

impl Clone for SlotKind {
    fn clone(&self) -> Self {
        match self {
            SlotKind::Empty => SlotKind::Empty,
            SlotKind::Virtual => SlotKind::Virtual,
            SlotKind::HeapTuple { desc, attr_descs, tid, tuple } => SlotKind::HeapTuple {
                desc: Rc::clone(desc),
                attr_descs: Rc::clone(attr_descs),
                tid: *tid,
                tuple: tuple.clone(),
            },
            SlotKind::BufferHeapTuple { .. } => {
                panic!("cannot clone BufferHeapTuple — call materialize() first")
            }
        }
    }
}

impl PartialEq for SlotKind {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SlotKind::Empty, SlotKind::Empty) => true,
            (SlotKind::Virtual, SlotKind::Virtual) => true,
            _ => false,
        }
    }
}

impl Eq for SlotKind {}

impl std::fmt::Debug for TupleSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TupleSlot")
            .field("kind", &self.kind)
            .field("tts_nvalid", &self.tts_nvalid)
            .field("ncols", &self.ncols())
            .finish()
    }
}

impl Clone for TupleSlot {
    fn clone(&self) -> Self {
        Self {
            kind: match &self.kind {
                SlotKind::BufferHeapTuple { .. } => SlotKind::Virtual,
                other => other.clone(),
            },
            tts_values: self.tts_values.iter().cloned().collect::<Vec<_>>(),
            tts_nvalid: self.tts_nvalid,
            decode_offset: 0,
            decoder: None,
        }
    }
}

impl PartialEq for TupleSlot {
    fn eq(&self, other: &Self) -> bool {
        self.tts_values == other.tts_values
    }
}

impl Eq for TupleSlot {}

#[derive(Debug, Clone, Default)]
pub struct NodeExecStats {
    pub loops: u64,
    pub rows: u64,
    pub total_time: Duration,
}

/// Trait for executor plan nodes, like PostgreSQL's ExecProcNode vtable.
/// Each node type implements this trait, and dispatch is via trait object.
///
/// `exec_proc_node` returns a borrowed `&mut TupleSlot` owned by the node.
/// Like PG's ExecProcNode, the caller must consume the slot before the next
/// call (the borrow checker enforces this).
pub trait PlanNode: std::fmt::Debug {
    fn exec_proc_node<'a>(
        &'a mut self,
        ctx: &mut super::ExecutorContext,
    ) -> Result<Option<&'a mut TupleSlot>, super::ExecError>;

    /// Re-borrow the slot from the last exec_proc_node call.
    /// Used by filter to return a reference to the child's slot
    /// after evaluating the predicate.
    fn current_slot(&mut self) -> Option<&mut TupleSlot>;

    /// Output column names for this node. Fixed for the lifetime of the query.
    fn column_names(&self) -> &[String];

    fn node_stats(&self) -> &NodeExecStats;
    fn node_stats_mut(&mut self) -> &mut NodeExecStats;
    fn node_label(&self) -> String;

    /// Format children for EXPLAIN output. The node itself is formatted by
    /// the caller; this method handles child nodes.
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>);
}

/// Executor plan state — a trait object for dynamic dispatch.
pub type PlanState = Box<dyn PlanNode>;

#[derive(Debug)]
pub struct ResultState {
    pub(crate) emitted: bool,
    pub(crate) slot: TupleSlot,
    pub(crate) stats: NodeExecStats,
}

pub struct SeqScanState {
    pub(crate) rel: RelFileLocator,
    pub(crate) column_names: Vec<String>,
    pub(crate) scan: Option<VisibleHeapScan>,
    /// Reusable slot, like PG's ss_ScanTupleSlot. Holds BufferHeapTuple
    /// with lazy decode into tts_values. The slot's `decoder` field holds
    /// the compiled tuple decoder (set once at plan start).
    pub(crate) slot: TupleSlot,
    /// Pushed-down qual, like PG's ExecSeqScanWithQual. When set, the scan
    /// evaluates the predicate inline and only returns qualifying tuples.
    /// Avoids a separate FilterState and its per-tuple vtable dispatch.
    pub(crate) qual: Option<super::expr::CompiledPredicate>,
    pub(crate) stats: NodeExecStats,
}

impl std::fmt::Debug for SeqScanState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeqScanState")
            .field("rel", &self.rel)
            .field("has_qual", &self.qual.is_some())
            .finish()
    }
}

pub struct FilterState {
    pub(crate) input: PlanState,
    pub(crate) predicate: Expr,
    pub(crate) compiled_predicate: super::expr::CompiledPredicate,
    pub(crate) stats: NodeExecStats,
}

impl std::fmt::Debug for FilterState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterState")
            .field("predicate", &self.predicate)
            .finish()
    }
}

#[derive(Debug)]
pub struct NestedLoopJoinState {
    pub(crate) left: PlanState,
    pub(crate) right: PlanState,
    pub(crate) on: Expr,
    pub(crate) combined_names: Vec<String>,
    pub(crate) right_rows: Option<Vec<TupleSlot>>,
    pub(crate) current_left: Option<TupleSlot>,
    pub(crate) right_index: usize,
    pub(crate) slot: TupleSlot,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct ProjectionState {
    pub(crate) input: PlanState,
    pub(crate) targets: Vec<TargetEntry>,
    pub(crate) column_names: Vec<String>,
    pub(crate) slot: TupleSlot,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct OrderByState {
    pub(crate) input: PlanState,
    pub(crate) items: Vec<OrderByEntry>,
    pub(crate) rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct LimitState {
    pub(crate) input: PlanState,
    pub(crate) limit: Option<usize>,
    pub(crate) offset: usize,
    pub(crate) skipped: usize,
    pub(crate) returned: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct AggregateState {
    pub(crate) input: PlanState,
    pub(crate) group_by: Vec<Expr>,
    pub(crate) accumulators: Vec<AggAccum>,
    pub(crate) having: Option<Expr>,
    pub(crate) output_columns: Vec<String>,
    pub(crate) result_rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    /// Reusable buffer for group-by key evaluation, allocated once at plan start.
    pub(crate) key_buffer: Vec<Value>,
    /// Compiled transition functions resolved at plan time, like PG's
    /// aggregate transfn pointers. Avoids per-tuple enum dispatch.
    pub(crate) trans_fns: Vec<fn(&mut AccumState, &Value)>,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct GenerateSeriesState {
    pub(crate) start: Expr,
    pub(crate) stop: Expr,
    pub(crate) step: Expr,
    pub(crate) current: i32,
    pub(crate) end: i32,
    pub(crate) step_val: i32,
    pub(crate) initialized: bool,
    pub(crate) slot: TupleSlot,
    pub(crate) column_names: Vec<String>,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct UnnestState {
    pub(crate) args: Vec<Expr>,
    pub(crate) output_columns: Vec<String>,
    pub(crate) rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct JsonTableFunctionState {
    pub(crate) kind: JsonTableFunction,
    pub(crate) arg: Expr,
    pub(crate) output_columns: Vec<String>,
    pub(crate) rows: Option<Vec<TupleSlot>>,
    pub(crate) next_index: usize,
    pub(crate) stats: NodeExecStats,
}

// --- PlanNode trait implementations ---

impl PlanNode for ResultState {
    fn exec_proc_node<'a>(&'a mut self, _ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            self.slot.kind = SlotKind::Virtual;
            self.slot.tts_values.clear();
            self.slot.tts_nvalid = 0;
            Ok(Some(&mut self.slot))
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { Some(&mut self.slot) }
    fn column_names(&self) -> &[String] { &[] }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Result".into() }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for SeqScanState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.scan.is_none() {
            self.scan = Some(heap_scan_begin_visible(
                &ctx.pool,
                ctx.client_id,
                self.rel,
                ctx.snapshot.clone(),
            )?);
        }

        let start = if ctx.timed { Some(Instant::now()) } else { None };

        loop {
            // Reborrow scan each iteration so the borrow checker can see that
            // state.slot is a separate field.
            let scan = self.scan.as_mut().unwrap();

            // Try to get the next tuple from the current page's visibility list.
            // Like PG's heapgettup_pagemode: no content lock per tuple — visibility
            // was already determined in heap_scan_prepare_next_page under a single
            // lock. The pin prevents eviction and tuple user data is immutable.
            if scan.has_page_tuples() {
                let buffer_id = scan.pinned_buffer_id().expect("buffer must be pinned");
                // SAFETY: buffer is pinned, visibility offsets were collected under
                // lock in prepare_next_page, and tuple user data is immutable.
                let page = unsafe { ctx.pool.page_unlocked(buffer_id) }
                    .expect("pinned buffer must be valid");

                if let Some((_tid, tuple_bytes)) = heap_scan_page_next_tuple(page, scan) {
                    let raw_ptr = tuple_bytes.as_ptr();
                    let raw_len = tuple_bytes.len();

                    let pin = scan.pinned_buffer_rc()
                        .expect("buffer must be pinned");

                    // Reset slot for new tuple (like PG's ExecStoreBufferHeapTuple)
                    self.slot.kind = SlotKind::BufferHeapTuple {
                        tuple_ptr: raw_ptr,
                        tuple_len: raw_len,
                        pin,
                    };
                    self.slot.tts_nvalid = 0;
                    self.slot.tts_values.clear();
                    self.slot.decode_offset = 0;

                    // Inline qual check (like PG's ExecScanExtended).
                    // Tuples that fail the predicate never leave the scan node.
                    if let Some(qual) = &self.qual {
                        if !qual(&mut self.slot, ctx)? {
                            continue;
                        }
                    }

                    if let Some(s) = start {
                        self.stats.loops += 1;
                        self.stats.total_time += s.elapsed();
                        self.stats.rows += 1;
                    }
                    return Ok(Some(&mut self.slot));
                }
            }

            // Current page exhausted — prepare the next page.
            let next: Result<Option<usize>, ExecError> =
                heap_scan_prepare_next_page(&*ctx.pool, ctx.client_id, &ctx.txns, scan);
            if next?.is_none() {
                heap_scan_end::<ExecError>(&*ctx.pool, ctx.client_id, scan)?;
                if let Some(s) = start {
                    self.stats.loops += 1;
                    self.stats.total_time += s.elapsed();
                }
                return Ok(None);
            }
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { Some(&mut self.slot) }
    fn column_names(&self) -> &[String] { &self.column_names }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { format!("Seq Scan on rel {}", self.rel.rel_number) }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for FilterState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let start = if ctx.timed { Some(Instant::now()) } else { None };
        loop {
            let slot = match self.input.exec_proc_node(ctx)? {
                Some(s) => s,
                None => {
                    if let Some(s) = start {
                        self.stats.loops += 1;
                        self.stats.total_time += s.elapsed();
                    }
                    return Ok(None);
                }
            };

            if (self.compiled_predicate)(slot, ctx)? {
                if let Some(s) = start {
                    self.stats.loops += 1;
                    self.stats.total_time += s.elapsed();
                    self.stats.rows += 1;
                }
                return Ok(self.input.current_slot());
            }
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { self.input.current_slot() }
    fn column_names(&self) -> &[String] { self.input.column_names() }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Filter".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for NestedLoopJoinState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.right_rows.is_none() {
            let mut rows = Vec::new();
            while let Some(slot) = self.right.exec_proc_node(ctx)? {
                let values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                rows.push(TupleSlot::virtual_row(values));
            }
            self.right_rows = Some(rows);
        }

        loop {
            if self.current_left.is_none() {
                match self.left.exec_proc_node(ctx)? {
                    Some(slot) => {
                        let values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                        self.current_left = Some(TupleSlot::virtual_row(values));
                        self.right_index = 0;
                    }
                    None => return Ok(None),
                }
            }

            let right_rows = self.right_rows.as_ref().unwrap();

            while self.right_index < right_rows.len() {
                let ri = self.right_index;
                self.right_index += 1;

                // Build combined slot from materialized left + right
                let left = self.current_left.as_ref().unwrap();
                let right = &right_rows[ri];
                let mut combined_values: Vec<Value> = left.tts_values.clone();
                combined_values.extend(right.tts_values.iter().cloned());
                let nvalid = combined_values.len();
                self.slot.tts_values = combined_values;
                self.slot.tts_nvalid = nvalid;
                self.slot.kind = SlotKind::Virtual;
                self.slot.decode_offset = 0;

                match eval_expr(&self.on, &mut self.slot, ctx)? {
                    Value::Bool(true) => return Ok(Some(&mut self.slot)),
                    Value::Bool(false) | Value::Null => {}
                    other => return Err(ExecError::NonBoolQual(other)),
                }
            }

            self.current_left = None;
        }
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { Some(&mut self.slot) }
    fn column_names(&self) -> &[String] { &self.combined_names }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Nested Loop".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.left, indent, analyze, lines);
        super::explain::format_explain_lines(&*self.right, indent, analyze, lines);
    }
}

impl PlanNode for OrderByState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.rows.is_none() {
            let mut rows = Vec::new();
            while let Some(slot) = self.input.exec_proc_node(ctx)? {
                let values = slot.values()?.iter().cloned().collect::<Vec<_>>();
                rows.push(TupleSlot::virtual_row(values));
            }

            let mut keyed_rows = Vec::with_capacity(rows.len());
            for mut row in rows {
                let mut keys = Vec::with_capacity(self.items.len());
                for item in &self.items {
                    keys.push(eval_expr(&item.expr, &mut row, ctx)?);
                }
                keyed_rows.push((keys, row));
            }

            keyed_rows.sort_by(|(left_keys, _), (right_keys, _)| {
                compare_order_by_keys(&self.items, left_keys, right_keys)
            });
            self.rows = Some(keyed_rows.into_iter().map(|(_, row)| row).collect());
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
    }
    fn column_names(&self) -> &[String] { self.input.column_names() }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Sort".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for LimitState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if let Some(limit) = self.limit {
            if self.returned >= limit {
                return Ok(None);
            }
        }

        while self.skipped < self.offset {
            if self.input.exec_proc_node(ctx)?.is_none() {
                return Ok(None);
            }
            self.skipped += 1;
        }

        let slot = self.input.exec_proc_node(ctx)?;
        if slot.is_some() {
            self.returned += 1;
        }
        Ok(slot)
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { self.input.current_slot() }
    fn column_names(&self) -> &[String] { self.input.column_names() }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Limit".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for ProjectionState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        let input_slot = match self.input.exec_proc_node(ctx)? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Evaluate projection targets. Materialize TextRef values since they
        // reference the input slot's page which may be overwritten on the next call.
        let mut values = Vec::with_capacity(self.targets.len());
        for target in &self.targets {
            values.push(eval_expr(&target.expr, input_slot, ctx)?.to_owned_value());
        }

        // Store in projection's own slot
        let nvalid = values.len();
        self.slot.tts_values = values;
        self.slot.tts_nvalid = nvalid;
        self.slot.kind = SlotKind::Virtual;
        self.slot.decode_offset = 0;
        Ok(Some(&mut self.slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { Some(&mut self.slot) }
    fn column_names(&self) -> &[String] { &self.column_names }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Projection".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for AggregateState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.result_rows.is_none() {
            let mut groups: Vec<AggGroup> = Vec::new();

            while let Some(slot) = self.input.exec_proc_node(ctx)? {
                self.key_buffer.clear();
                for expr in &self.group_by {
                    self.key_buffer.push(eval_expr(expr, slot, ctx)?);
                }

                let group_idx = groups
                    .iter()
                    .position(|g| g.key_values == self.key_buffer)
                    .unwrap_or_else(|| {
                        let accum_states = self
                            .accumulators
                            .iter()
                            .map(|a| AccumState::new(a.func, a.distinct, a.sql_type))
                            .collect();
                        groups.push(AggGroup {
                            key_values: self.key_buffer.clone(),
                            accum_states,
                        });
                        groups.len() - 1
                    });

                let group = &mut groups[group_idx];
                static NO_ARG: Value = Value::Null;
                for (i, accum) in self.accumulators.iter().enumerate() {
                    let value: &Value = if let Some(arg) = &accum.arg {
                        &eval_expr(arg, slot, ctx)?
                    } else {
                        &NO_ARG
                    };
                    (self.trans_fns[i])(&mut group.accum_states[i], value);
                }
            }

            if groups.is_empty() && self.group_by.is_empty() {
                let accum_states = self
                    .accumulators
                    .iter()
                    .map(|a| AccumState::new(a.func, a.distinct, a.sql_type))
                    .collect();
                groups.push(AggGroup {
                    key_values: Vec::new(),
                    accum_states,
                });
            }

            let mut result_rows = Vec::new();
            for group in &groups {
                let mut row_values = group.key_values.clone();
                for accum_state in &group.accum_states {
                    row_values.push(accum_state.finalize());
                }

                if let Some(having) = &self.having {
                    let mut having_slot = TupleSlot::virtual_row(row_values.clone());
                    match eval_expr(having, &mut having_slot, ctx)? {
                        Value::Bool(true) => {}
                        Value::Bool(false) | Value::Null => continue,
                        other => return Err(ExecError::NonBoolQual(other)),
                    }
                }

                result_rows.push(TupleSlot::virtual_row(row_values));
            }

            self.result_rows = Some(result_rows);
        }

        let rows = self.result_rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.result_rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
    }
    fn column_names(&self) -> &[String] { &self.output_columns }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Aggregate".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for GenerateSeriesState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if !self.initialized {
            let mut dummy = TupleSlot::empty(0);
            self.current = match eval_expr(&self.start, &mut dummy, ctx)? {
                Value::Int32(v) => v,
                other => return Err(ExecError::TypeMismatch { op: "generate_series start", left: other, right: Value::Null }),
            };
            self.end = match eval_expr(&self.stop, &mut dummy, ctx)? {
                Value::Int32(v) => v,
                other => return Err(ExecError::TypeMismatch { op: "generate_series stop", left: other, right: Value::Null }),
            };
            self.step_val = match eval_expr(&self.step, &mut dummy, ctx)? {
                Value::Int32(v) => v,
                other => return Err(ExecError::TypeMismatch { op: "generate_series step", left: other, right: Value::Null }),
            };
            self.initialized = true;
        }

        let done = if self.step_val > 0 {
            self.current > self.end
        } else if self.step_val < 0 {
            self.current < self.end
        } else {
            return Err(ExecError::TypeMismatch { op: "generate_series step must be non-zero", left: Value::Int32(0), right: Value::Null });
        };

        if done {
            return Ok(None);
        }

        self.slot.kind = SlotKind::Virtual;
        self.slot.tts_values.clear();
        self.slot.tts_values.push(Value::Int32(self.current));
        self.slot.tts_nvalid = 1;
        self.current += self.step_val;
        Ok(Some(&mut self.slot))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> { Some(&mut self.slot) }
    fn column_names(&self) -> &[String] { &self.column_names }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Function Scan on generate_series".into() }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for UnnestState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            let mut arrays = Vec::with_capacity(self.args.len());
            let mut max_len = 0usize;
            for arg in &self.args {
                match eval_expr(arg, &mut dummy, ctx)? {
                    Value::Null => arrays.push(None),
                    Value::Array(values) => {
                        max_len = max_len.max(values.len());
                        arrays.push(Some(values));
                    }
                    other => {
                        return Err(ExecError::TypeMismatch {
                            op: "unnest",
                            left: other,
                            right: Value::Null,
                        });
                    }
                }
            }

            let mut rows = Vec::with_capacity(max_len);
            for idx in 0..max_len {
                let mut row = Vec::with_capacity(arrays.len());
                for array in &arrays {
                    match array {
                        Some(values) => row.push(values.get(idx).cloned().unwrap_or(Value::Null)),
                        None => row.push(Value::Null),
                    }
                }
                rows.push(TupleSlot::virtual_row(row));
            }
            self.rows = Some(rows);
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }

    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
    }

    fn column_names(&self) -> &[String] { &self.output_columns }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Function Scan on unnest".into() }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for JsonTableFunctionState {
    fn exec_proc_node<'a>(&'a mut self, ctx: &mut ExecutorContext) -> Result<Option<&'a mut TupleSlot>, ExecError> {
        if self.rows.is_none() {
            let mut dummy = TupleSlot::empty(0);
            self.rows = Some(crate::backend::executor::exec_expr::eval_json_table_function(
                self.kind,
                &self.arg,
                &mut dummy,
                ctx,
            )?);
        }

        let rows = self.rows.as_mut().unwrap();
        if self.next_index >= rows.len() {
            return Ok(None);
        }

        let idx = self.next_index;
        self.next_index += 1;
        Ok(Some(&mut rows[idx]))
    }
    fn current_slot(&mut self) -> Option<&mut TupleSlot> {
        let rows = self.rows.as_mut()?;
        let idx = self.next_index.checked_sub(1)?;
        rows.get_mut(idx)
    }
    fn column_names(&self) -> &[String] { &self.output_columns }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Function Scan on json".into() }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl TupleSlot {
    pub fn from_heap_tuple(
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
    ) -> Self {
        let ncols = desc.columns.len();
        Self {
            kind: SlotKind::HeapTuple { desc, attr_descs, tid, tuple },
            tts_values: Vec::with_capacity(ncols),
            tts_nvalid: 0,
            decode_offset: 0,
            decoder: None,
        }
    }

    pub fn virtual_row(values: Vec<Value>) -> Self {
        let nvalid = values.len();
        Self {
            kind: SlotKind::Virtual,
            tts_values: values,
            tts_nvalid: nvalid,
            decode_offset: 0,
            decoder: None,
        }
    }

    pub(crate) fn empty(ncols: usize) -> Self {
        Self {
            kind: SlotKind::Empty,
            tts_values: Vec::with_capacity(ncols),
            tts_nvalid: 0,
            decode_offset: 0,
            decoder: None,
        }
    }

    /// Read a fixed-offset int32 directly from raw tuple bytes, like PG's
    /// heap_getattr fast path. Bypasses the full decode machinery. Returns
    /// None if the slot is not a BufferHeapTuple.
    #[inline]
    pub(crate) fn get_fixed_int32(&self, data_offset: usize) -> Option<i32> {
        if let SlotKind::BufferHeapTuple { tuple_ptr, tuple_len, .. } = &self.kind {
            let bytes = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
            let hoff = bytes[22] as usize;
            let start = hoff + data_offset;
            if start + 4 <= bytes.len() {
                return Some(i32::from_le_bytes([
                    bytes[start], bytes[start + 1], bytes[start + 2], bytes[start + 3],
                ]));
            }
        }
        None
    }

    /// Number of columns in this slot.
    pub(crate) fn ncols(&self) -> usize {
        match &self.kind {
            SlotKind::HeapTuple { desc, .. } => desc.columns.len(),
            SlotKind::BufferHeapTuple { .. } => self.decoder.as_ref().expect("BufferHeapTuple requires decoder").ncols(),
            SlotKind::Virtual | SlotKind::Empty => self.tts_values.len(),
        }
    }

    /// Convert to a self-contained Virtual slot, decoding all columns and
    /// materializing TextRef → owned Text. Releases the buffer pin.
    pub fn materialize(mut self) -> Result<Self, super::ExecError> {
        self.values()?;
        Value::materialize_all(&mut self.tts_values);
        Ok(Self {
            kind: SlotKind::Virtual,
            tts_values: self.tts_values,
            tts_nvalid: self.tts_nvalid,
            decode_offset: 0,
            decoder: None,
        })
    }

    pub fn tid(&self) -> Option<ItemPointerData> {
        match &self.kind {
            SlotKind::HeapTuple { tid, .. } => Some(*tid),
            _ => None,
        }
    }

    /// Decode all columns. Like PG's slot_getallattrs().
    pub fn values(&mut self) -> Result<&[Value], super::ExecError> {
        let ncols = self.ncols();
        self.slot_getsomeattrs(ncols)
    }

    /// Decode columns 0..natts. Like PG's slot_getsomeattrs(slot, natts).
    /// Columns already decoded (< tts_nvalid) are skipped.
    pub fn slot_getsomeattrs(&mut self, natts: usize) -> Result<&[Value], super::ExecError> {
        if self.tts_nvalid >= natts {
            return Ok(&self.tts_values[..natts]);
        }
        match &self.kind {
            SlotKind::Virtual => {
                // Virtual: tts_values is authoritative
                Ok(&self.tts_values[..natts])
            }
            SlotKind::BufferHeapTuple { tuple_ptr, tuple_len, .. } => {
                let (ptr, len) = (*tuple_ptr, *tuple_len);
                let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
                let decoder = self.decoder.as_ref().expect("BufferHeapTuple requires decoder");
                decoder.decode_range(bytes, &mut self.tts_values, self.tts_nvalid, natts, &mut self.decode_offset)?;
                self.tts_nvalid = natts;
                Ok(&self.tts_values[..natts])
            }
            SlotKind::HeapTuple { desc, attr_descs, tuple, .. } => {
                // HeapTuple: decode all columns at once via deform()
                let raw = tuple.deform(attr_descs)?;
                self.tts_values.clear();
                for (column, datum) in desc.columns.iter().zip(raw.into_iter()) {
                    self.tts_values.push(decode_value(column, datum)?);
                }
                self.tts_nvalid = self.tts_values.len();
                Ok(&self.tts_values[..natts])
            }
            SlotKind::Empty => {
                panic!("cannot get attrs from empty slot")
            }
        }
    }

    /// Get a single column value, decoding only up to that column.
    /// Like PG's slot_getattr().
    pub fn get_attr(&mut self, index: usize) -> Result<&Value, super::ExecError> {
        self.slot_getsomeattrs(index + 1)?;
        Ok(&self.tts_values[index])
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, super::ExecError> {
        self.values()?;
        Value::materialize_all(&mut self.tts_values);
        Ok(self.tts_values)
    }
}
