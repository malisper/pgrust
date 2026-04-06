use crate::access::heap::am::VisibleHeapScan;
use crate::access::heap::tuple::{AttributeDesc, HeapTuple, ItemPointerData};
use crate::compact_string::CompactString;
use crate::{OwnedBufferPin, RelFileLocator, SmgrStorageBackend};
use std::rc::Rc;
use std::time::Duration;

use super::expr::decode_value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    Int32,
    Text,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    pub name: String,
    pub storage: AttributeDesc,
    pub ty: ScalarType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDesc {
    pub columns: Vec<ColumnDesc>,
}

impl RelationDesc {
    pub fn attribute_descs(&self) -> Vec<AttributeDesc> {
        self.columns.iter().map(|c| c.storage.clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Int32(i32),
    Float64(f64),
    Text(CompactString),
    /// Raw pointer to on-page text bytes. Valid while the buffer page is pinned
    /// (the slot's `Rc<OwnedBufferPin>` keeps the pin alive). User data on the
    /// page is immutable after insertion.
    TextRef(*const u8, u32),
    Bool(bool),
    Null,
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
            Value::TextRef(ptr, len) => {
                let s = unsafe {
                    std::str::from_utf8_unchecked(std::slice::from_raw_parts(*ptr, *len as usize))
                };
                Value::Text(CompactString::new(s))
            }
            other => other.clone(),
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
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Int32(a), Value::Int32(b)) => a == b,
            (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Null, Value::Null) => true,
            (a, b) if a.as_text().is_some() && b.as_text().is_some() => {
                a.as_text().unwrap() == b.as_text().unwrap()
            }
            _ => false,
        }
    }
}

impl Eq for Value {}

// SAFETY: TextRef points to immutable user data on a pinned buffer page.
// The pin (via Rc<OwnedBufferPin>) ensures the page stays alive. The data
// is never written after insertion (heap_page_replace_tuple only writes headers).
unsafe impl Send for Value {}
unsafe impl Sync for Value {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetEntry {
    pub name: String,
    pub expr: Expr,
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
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggAccum {
    pub func: AggFunc,
    pub arg: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    Column(usize),
    Const(Value),
    Add(Box<Expr>, Box<Expr>),
    Negate(Box<Expr>),
    Eq(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    Random,
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
        output_columns: Vec<String>,
    },
}

impl Plan {
    /// Extract output column names from the plan tree.
    pub fn column_names(&self) -> Vec<String> {
        match self {
            Plan::Result => vec![],
            Plan::SeqScan { desc, .. } => desc.columns.iter().map(|c| c.name.clone()).collect(),
            Plan::Filter { input, .. } | Plan::OrderBy { input, .. } | Plan::Limit { input, .. } => {
                input.column_names()
            }
            Plan::Projection { targets, .. } => targets.iter().map(|t| t.name.clone()).collect(),
            Plan::Aggregate { output_columns, .. } => output_columns.clone(),
            Plan::NestedLoopJoin { left, right, .. } => {
                let mut names = left.column_names();
                names.extend(right.column_names());
                names
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleSlot {
    pub(crate) column_names: Rc<[String]>,
    pub(crate) source: SlotSource,
}

pub(crate) enum SlotSource {
    Physical {
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
        materialized: Option<Vec<Value>>,
    },
    Virtual {
        values: Vec<Value>,
    },
    /// Zero-copy slot: holds a raw pointer to tuple bytes on a pinned buffer
    /// page, plus an owned pin that keeps the buffer alive. User data on the
    /// page is immutable (heap_page_replace_tuple only writes headers).
    ///
    /// The `OwnedBufferPin` is an independent pin taken via
    /// `increment_buffer_pin`, so the slot remains valid even after the scan
    /// advances to the next page and releases its own pin.
    BufferHeap {
        tuple_ptr: *const u8,
        tuple_len: usize,
        pin: Rc<OwnedBufferPin<SmgrStorageBackend>>,
        decoder: Rc<super::tuple_decoder::CompiledTupleDecoder>,
        materialized: Option<Vec<Value>>,
    },
    /// Zero-allocation slot: references a pre-allocated values buffer owned by
    /// SeqScanState. The buffer contains decoded Values (which may include
    /// TextRef pointers to on-page bytes). The pin keeps the page alive for
    /// any TextRef values. The values_ptr is valid until the next exec_seq_scan
    /// call, which only happens after the caller consumes/drops this slot.
    ScanBuf {
        values_ptr: *const Value,
        values_len: usize,
        pin: Rc<OwnedBufferPin<SmgrStorageBackend>>,
    },
}

impl std::fmt::Debug for SlotSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SlotSource::Physical { desc, tid, materialized, .. } => f
                .debug_struct("Physical")
                .field("desc_cols", &desc.columns.len())
                .field("tid", tid)
                .field("materialized", &materialized.is_some())
                .finish(),
            SlotSource::Virtual { values } => f
                .debug_struct("Virtual")
                .field("len", &values.len())
                .finish(),
            SlotSource::BufferHeap { tuple_len, pin, materialized, .. } => f
                .debug_struct("BufferHeap")
                .field("tuple_len", tuple_len)
                .field("buffer_id", &pin.buffer_id())
                .field("materialized", &materialized.is_some())
                .finish(),
            SlotSource::ScanBuf { values_len, pin, .. } => f
                .debug_struct("ScanBuf")
                .field("values_len", values_len)
                .field("buffer_id", &pin.buffer_id())
                .finish(),
        }
    }
}

impl Clone for SlotSource {
    fn clone(&self) -> Self {
        match self {
            SlotSource::Physical { desc, attr_descs, tid, tuple, materialized } => {
                SlotSource::Physical {
                    desc: Rc::clone(desc),
                    attr_descs: Rc::clone(attr_descs),
                    tid: *tid,
                    tuple: tuple.clone(),
                    materialized: materialized.clone(),
                }
            }
            SlotSource::Virtual { values } => SlotSource::Virtual { values: values.clone() },
            SlotSource::BufferHeap { materialized: Some(values), .. } => {
                // Cloning a BufferHeap forces materialization — the clone becomes
                // a self-contained Virtual slot so it doesn't depend on the pin.
                SlotSource::Virtual { values: values.clone() }
            }
            SlotSource::BufferHeap { .. } => {
                panic!("cannot clone unmaterialized BufferHeap slot — call materialize() first")
            }
            SlotSource::ScanBuf { values_ptr, values_len, .. } => {
                // Clone must materialize — the clone may outlive the values buffer.
                let slice = unsafe { std::slice::from_raw_parts(*values_ptr, *values_len) };
                SlotSource::Virtual {
                    values: slice.iter().map(Value::to_owned_value).collect(),
                }
            }
        }
    }
}

impl PartialEq for SlotSource {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SlotSource::Virtual { values: a }, SlotSource::Virtual { values: b }) => a == b,
            _ => false,
        }
    }
}

impl Eq for SlotSource {}

#[derive(Debug, Clone, Default)]
pub struct NodeExecStats {
    pub loops: u64,
    pub rows: u64,
    pub total_time: Duration,
}

/// Trait for executor plan nodes, like PostgreSQL's ExecProcNode vtable.
/// Each node type implements this trait, and dispatch is via trait object.
pub trait PlanNode: std::fmt::Debug {
    fn exec_proc_node(
        &mut self,
        ctx: &mut super::ExecutorContext,
    ) -> Result<Option<TupleSlot>, super::ExecError>;

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
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct SeqScanState {
    pub(crate) rel: RelFileLocator,
    pub(crate) column_names: Rc<[String]>,
    pub(crate) scan: Option<VisibleHeapScan>,
    pub(crate) decoder: Rc<super::tuple_decoder::CompiledTupleDecoder>,
    /// Pre-allocated values buffer, reused across tuples. Contains TextRef
    /// pointers to on-page bytes (valid while pinned).
    pub(crate) values_buf: Vec<Value>,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct FilterState {
    pub(crate) input: PlanState,
    pub(crate) predicate: Expr,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct NestedLoopJoinState {
    pub(crate) left: PlanState,
    pub(crate) right: PlanState,
    pub(crate) on: Expr,
    pub(crate) right_rows: Option<Vec<TupleSlot>>,
    pub(crate) current_left: Option<TupleSlot>,
    pub(crate) right_index: usize,
    pub(crate) stats: NodeExecStats,
}

#[derive(Debug)]
pub struct ProjectionState {
    pub(crate) input: PlanState,
    pub(crate) targets: Vec<TargetEntry>,
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
    pub(crate) stats: NodeExecStats,
}

// --- PlanNode trait implementations ---

impl PlanNode for ResultState {
    fn exec_proc_node(&mut self, _ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_result(self)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Result".into() }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for SeqScanState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_seq_scan(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { format!("Seq Scan on rel {}", self.rel.rel_number) }
    fn explain_children(&self, _indent: usize, _analyze: bool, _lines: &mut Vec<String>) {}
}

impl PlanNode for FilterState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_filter(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Filter".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for NestedLoopJoinState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_nested_loop_join(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Nested Loop".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.left, indent, analyze, lines);
        super::explain::format_explain_lines(&*self.right, indent, analyze, lines);
    }
}

impl PlanNode for OrderByState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_order_by(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Sort".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for LimitState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_limit(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Limit".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for ProjectionState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_projection(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Projection".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl PlanNode for AggregateState {
    fn exec_proc_node(&mut self, ctx: &mut super::ExecutorContext) -> Result<Option<TupleSlot>, super::ExecError> {
        super::exec_aggregate(self, ctx)
    }
    fn node_stats(&self) -> &NodeExecStats { &self.stats }
    fn node_stats_mut(&mut self) -> &mut NodeExecStats { &mut self.stats }
    fn node_label(&self) -> String { "Aggregate".into() }
    fn explain_children(&self, indent: usize, analyze: bool, lines: &mut Vec<String>) {
        super::explain::format_explain_lines(&*self.input, indent, analyze, lines);
    }
}

impl TupleSlot {
    pub fn from_heap_tuple(
        desc: Rc<RelationDesc>,
        attr_descs: Rc<[AttributeDesc]>,
        column_names: Rc<[String]>,
        tid: ItemPointerData,
        tuple: HeapTuple,
    ) -> Self {
        Self {
            column_names,
            source: SlotSource::Physical {
                desc,
                attr_descs,
                tid,
                tuple,
                materialized: None,
            },
        }
    }

    pub fn virtual_row(column_names: Rc<[String]>, values: Vec<Value>) -> Self {
        Self {
            column_names,
            source: SlotSource::Virtual { values },
        }
    }

    pub fn column_names(&self) -> &Rc<[String]> {
        &self.column_names
    }

    /// Create a zero-copy slot that points directly at on-page tuple bytes.
    ///
    /// Takes an independent pin on the buffer via `OwnedBufferPin`, so the
    /// slot remains valid even after the scan releases its own pin on the page.
    ///
    /// SAFETY: `tuple_ptr` must point to valid tuple bytes on the given
    /// buffer page, and the buffer must currently be pinned by the caller.
    pub(crate) unsafe fn from_buffer_heap(
        column_names: Rc<[String]>,
        tuple_ptr: *const u8,
        tuple_len: usize,
        pin: Rc<OwnedBufferPin<SmgrStorageBackend>>,
        decoder: Rc<super::tuple_decoder::CompiledTupleDecoder>,
    ) -> Self {
        Self {
            column_names,
            source: SlotSource::BufferHeap {
                tuple_ptr,
                tuple_len,
                pin,
                decoder,
                materialized: None,
            },
        }
    }

    /// Convert to a self-contained Virtual slot, decoding from the raw pointer
    /// if needed. Releases the buffer pin when done.
    pub fn materialize(mut self) -> Result<Self, super::ExecError> {
        match &mut self.source {
            SlotSource::Virtual { .. } | SlotSource::Physical { .. } => Ok(self),
            SlotSource::BufferHeap { materialized: Some(_), .. } => {
                let SlotSource::BufferHeap { materialized, .. } = self.source else { unreachable!() };
                let mut values = materialized.unwrap();
                Value::materialize_all(&mut values);
                Ok(Self {
                    column_names: self.column_names,
                    source: SlotSource::Virtual { values },
                })
            }
            SlotSource::BufferHeap { tuple_ptr, tuple_len, decoder, materialized, .. } => {
                let bytes = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
                let mut values = Vec::new();
                decoder.decode_into(bytes, &mut values)?;
                Value::materialize_all(&mut values);
                *materialized = Some(values);
                let SlotSource::BufferHeap { materialized, .. } = self.source else { unreachable!() };
                Ok(Self {
                    column_names: self.column_names,
                    source: SlotSource::Virtual { values: materialized.unwrap() },
                })
            }
            SlotSource::ScanBuf { values_ptr, values_len, .. } => {
                let slice = unsafe { std::slice::from_raw_parts(*values_ptr, *values_len) };
                Ok(Self {
                    column_names: self.column_names,
                    source: SlotSource::Virtual {
                        values: slice.iter().map(Value::to_owned_value).collect(),
                    },
                })
            }
        }
    }

    pub fn tid(&self) -> Option<ItemPointerData> {
        match &self.source {
            SlotSource::Physical { tid, .. } => Some(*tid),
            SlotSource::Virtual { .. }
            | SlotSource::BufferHeap { .. }
            | SlotSource::ScanBuf { .. } => None,
        }
    }

    pub fn values(&mut self) -> Result<&[Value], super::ExecError> {
        match &mut self.source {
            SlotSource::Virtual { values } => Ok(values.as_slice()),
            SlotSource::Physical {
                desc,
                attr_descs,
                tuple,
                materialized,
                ..
            } => {
                if materialized.is_none() {
                    let raw = tuple.deform(attr_descs)?;
                    let mut values = Vec::with_capacity(desc.columns.len());
                    for (column, datum) in desc.columns.iter().zip(raw.into_iter()) {
                        values.push(decode_value(column, datum)?);
                    }
                    *materialized = Some(values);
                }
                Ok(materialized.as_ref().unwrap().as_slice())
            }
            SlotSource::BufferHeap { tuple_ptr, tuple_len, decoder, materialized, .. } => {
                if materialized.is_none() {
                    let bytes = unsafe { std::slice::from_raw_parts(*tuple_ptr, *tuple_len) };
                    let mut values = Vec::new();
                    decoder.decode_into(bytes, &mut values)?;
                    *materialized = Some(values);
                }
                Ok(materialized.as_ref().unwrap().as_slice())
            }
            SlotSource::ScanBuf { values_ptr, values_len, .. } => {
                Ok(unsafe { std::slice::from_raw_parts(*values_ptr, *values_len) })
            }
        }
    }

    pub fn into_values(mut self) -> Result<Vec<Value>, super::ExecError> {
        match &self.source {
            SlotSource::ScanBuf { values_ptr, values_len, .. } => {
                let slice = unsafe { std::slice::from_raw_parts(*values_ptr, *values_len) };
                return Ok(slice.iter().map(Value::to_owned_value).collect());
            }
            _ => {}
        }
        self.values()?;
        match self.source {
            SlotSource::Virtual { values } => Ok(values),
            SlotSource::Physical { materialized: Some(mut values), .. } => {
                Value::materialize_all(&mut values);
                Ok(values)
            }
            SlotSource::Physical { materialized: None, .. } => unreachable!("values() just materialized"),
            SlotSource::BufferHeap { materialized: Some(mut values), .. } => {
                Value::materialize_all(&mut values);
                Ok(values)
            }
            SlotSource::BufferHeap { materialized: None, .. } => unreachable!("values() just materialized"),
            SlotSource::ScanBuf { .. } => unreachable!("handled above"),
        }
    }
}
